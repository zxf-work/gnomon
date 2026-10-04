"""A node scan pays for the rows it answers with, not for the store it sits in.

`nodes_columnar()` used to be a flat `for i in 0..rows` sweep over every node
segment (`NativeStore::scan_node_versions`). This had already moved the
*materialization* below the filter — a row's uid, vid and label are built only
if it survives — but the sweep itself still read every stored version's integer
columns, ran the valid-time comparison on each, binary-searched `tt_s_runs` for
each survivor's transaction time, and probed the close index for each. The
comment on that loop said node segments "carry no belief index, so there is
nothing to prune by", because the node store is identity-clustered.

That conflated two different indexes. What is identity-clustered is the
name/uid side — `resolve_entities` still sweeps, and that remains a known limitation. The
*rows* of a node segment are written sorted by `(vt_s, vid)`: `NodeRow::sort_key`
is the same key `EdgeRow` uses, and node segments carry the same `tt_s_runs`,
the same folded `(vt_min, vt_max, vt_e_max)` zone map and the same close
sidecar. Every input the edge cursor narrows with was already there and was
being left on the floor.

The node scan now goes through the same `ScanSet`/`select_range` path the edge
scan does: prune from the header, binary-search the valid-time window, subtract
the closed rows from the verdict ranges, then materialize only what is left.
The answer is unchanged row for row — `scan_node_versions_agrees_with_the_filtered_listing`
in `read.rs` is the oracle for that and was not touched.

**What this file gates, and why it is shaped the way it is.** The obvious
statistic — corrected-vs-clean per-row wall clock, the shape
`test_corrected_scan_scaling.py` uses for edges — does not work here, and
saying so is part of the result. A node row costs ~0.32 us to materialize (a
dictionary lookup, a 24-hex identity, a string-table read) against ~0.02 us to
sweep, so the swept rows are ~6 % of the bill and no ratio built on them
separates. Measured: the corrected-vs-clean ratio is 1.03x before this change
and 1.00x after. A gate on that would never have failed on the pre-fix engine,
which makes it an assertion about nothing.

So the gate is built on the two things that do separate:

1. `rows_examined`, the engine's own counter — exact, host-independent, and
   wrong by a factor of 12 to 64 on the pre-fix engine. This is the primary
   assertion here, and it is *stronger* than a host-independent ratio, not a
   substitute for one.
2. A wall-clock ratio that puts the sweep where it can be seen: hold the number
   of rows the answer contains fixed and grow the store behind it. Pre-fix the
   sweep is O(store) for an O(answer) result, so per-returned-row cost grows
   with the store; post-fix it does not.

These run against the **native** engine specifically, whatever
GNOMON_TEST_BACKEND says — verdict ranges and close sidecars are native segment
concepts and a DuckDB run would pass every test here vacuously.
"""

from __future__ import annotations

import json
import statistics
import time
from pathlib import Path

import pytest

# --- the thresholds, and the control that set them ------------------------ #
#
# The pre-fix engine is the commit before the change, built from its own
# worktree; the post-fix engine is the change itself. Same fixtures, same
# statistics, three rounds each, on darwin/arm64 and on the measurement host.
# `rows_examined` is exact and identical on both; the wall clock is not, so
# (2) carries both.
#
# (1) The primary gate, `rows_examined`, exact on both engines:
#
#     case                                          pre-fix   post-fix
#     full scan, clean store (24,000 rows)           24,000    24,000
#     full scan, 5%-corrected store (25,200 stored)  25,200    24,000
#     500-row valid-time window over 32,000 rows     32,000       500
#     historical as_of selecting 2,000 of 64,000     64,000     2,000
#
# (2) The wall-clock gate below: a historical `as_of` that selects exactly
#     BELIEVED rows, on a SMALL-row store and a BIG-row store. Both answers
#     are the same size, so per-returned-row cost is a direct reading of what
#     the scan paid for rows it did not return. Measured on **two hosts**,
#     three rounds each:
#
#     host              engine     small us/row    big us/row      ratio
#     darwin/arm64      pre-fix    0.3931-0.4059   0.9139-0.9328   2.28x-2.34x
#     darwin/arm64      post-fix   0.3000-0.3263   0.3291-0.3352   1.03x-1.12x
#     dev workstation (Linux, 20t) pre-fix    0.5116-0.5169   1.4210-1.4488   2.77x-2.81x
#     dev workstation (Linux, 20t) post-fix   0.4314-0.4369   0.4795-0.4833   1.11x
#
#     and the model behind it checks out on both: per-returned-row cost is
#     M + S*(store/returned) with M ~ 0.32 us of materialization and
#     S ~ 0.018 us of sweep on darwin, which predicts 2.28x at these sizes.
#     The defect is *larger* on the slower host (2.8x vs 2.3x), the opposite
#     of the edge-side residual, because here the host's extra cost lands on
#     the swept rows rather than on a fixed per-range charge.
#
# (3) For the record, the statistic this file deliberately does **not** gate
#     on — corrected-vs-clean full-scan per-row cost, 24,000 rows, 5 %
#     corrections folded into the sidecar:
#
#              clean us/row    corrected us/row   ratio
#     pre-fix  0.3381-0.3417   0.3501-0.3520      1.026x - 1.040x
#     post-fix 0.3035-0.3053   0.3045-0.3059      0.999x - 1.008x
#
#     The change is real and in the right direction — both configurations get 11-13 %
#     faster, because the per-row `tt_s_at` binary search and close probe are
#     gone — but a 1.03x defect cannot carry a threshold. It is asserted here
#     through `rows_examined` instead, where the same claim reads 25,200
#     against 24,000.
#
# MAX_SCALING_RATIO = 1.6 sits 1.44x above the noisiest healthy rep across
# both hosts (1.11x) and 1.42x below the weakest defect rep across both
# (2.28x) — the balance `test_compacted_layout_scaling.py`'s 2.0 strikes
# between its 1.10x and 3.49x. Two hosts land inside it with matching room on
# each side, and a regression has to close most of a 2.1x gap before it slips
# through.
MAX_SCALING_RATIO = 1.6

# The belt: absolute per-returned-row cost on the big configuration. Across the two
# hosts, healthy spans 0.329-0.483 us/row and the defect spans 0.914-1.449,
# so the entire usable window for an absolute bound is 1.9x wide.
#
# This bound was 0.60 when it had only the darwin measurement behind it, which
# would have left 1.24x of headroom over the dev workstation's 0.483 — a gate that passes
# today and flakes the first time the measurement host is busy. 0.70 sits
# 1.45x above the slowest healthy rep and 1.31x below the fastest defect rep,
# which is the best balance the window allows.
#
# It is thinner in both directions than the ratio, because materialization
# sets a per-row floor that no amount of narrowing moves. The ratio above is
# the gate and `rows_examined` is the proof; this catches only the case a
# ratio cannot see, both configurations getting slow together.
MAX_ABS_US_PER_ROW = 0.70

N_SMALL = 8_000
N_BIG = 64_000
BELIEVED = 2_000  # rows the historical as_of selects, on *both* configurations
N_CORRECTED = 24_000
CORRECTION_PCT = 5.0
WINDOW_ROWS = 500
ROWS_PER_BATCH = 500
REPS = 5


def _native_adapter(tmp):
    """The native engine, regardless of GNOMON_TEST_BACKEND (see module doc)."""
    native = pytest.importorskip(
        "gnomon.storage.native",
        reason="the node-scan gate measures the native scan path",
    )
    a = native.NativeAdapter(tmp)
    a.paranoid = False  # the paranoid disjointness check is O(versions) per op
    return a


def _node_ops(vts, mark: int):
    return [
        {"op": "assert_node", "uid": f"n{vt}", "label": "N",
         "props": {"v": vt * mark}, "vt_s": vt, "vt_e": vt + 1}
        for vt in vts
    ]


def _build(tmp: Path, n_rows: int, corr_pct: float = 0.0):
    """An interleaved, compacted node store, optionally corrected.

    Generation `g` writes `vt = g, g + n_gens, ...`, so `compact()`'s global
    `(vt_s, vid)` sort visits every generation in rotation and the compacted
    segment ends up with one `tt_s` run per row. That is deliberate: the
    pre-fix sweep binary-searched `tt_s_runs` once per surviving row, so a
    tidy layout would have hidden part of what this change removes.

    Returns `(adapter, tt_marks)`; `tt_marks[k]` is the transaction time after
    generation `k` committed, which is how a historical `as_of` selecting an
    exact number of rows is addressed.
    """
    a = _native_adapter(tmp)
    n_gens = n_rows // ROWS_PER_BATCH
    assert n_gens >= 8, f"n_rows={n_rows} at {ROWS_PER_BATCH}/batch gives {n_gens} gens"
    tt = 1000
    marks = []
    for g in range(n_gens):
        tt += 1
        a.begin()
        a.apply_ops(_node_ops([i * n_gens + g for i in range(ROWS_PER_BATCH)], 1), tt)
        a.commit()
        marks.append(tt)
    n_corr = round(n_rows * corr_pct / 100.0)
    if n_corr:
        step = max(1, n_rows // n_corr)
        tt += 1
        a.begin()
        a.apply_ops(_node_ops(range(0, n_rows, step), -1), tt)
        a.commit()
        marks.append(tt)
    a.compact()
    return a, marks


def _folded_closes(store: Path) -> tuple[int, bool]:
    """`(closes folded into sidecars, every live node segment all_current)`.

    From the manifest, not from `verify()`: `verify()["closes"]` counts
    entries in close *run* files, which `compact()` has emptied on both configurations.
    """
    generation = int((store / "CURRENT").read_text().split()[0])
    m = json.loads((store / "manifests" / f"{generation:020d}.json").read_text())
    entries = m["node_store"]
    assert entries, f"no live node segments in generation {generation} of {store}"
    return (
        sum(e["n_closed_folded"] for e in entries),
        all(e["all_current"] for e in entries),
    )


def _scan_stats(adapter, **kw):
    from gnomon.core.model import OPEN_END, clamp_tt

    kw.setdefault("as_of_tt", clamp_tt(OPEN_END))
    kw.setdefault("vt_min", None)
    kw.setdefault("vt_max", None)
    return adapter._store.scan_nodes(**kw)


# --- 1. the primary gate: what the scan examines --------------------------- #


def test_a_corrected_node_store_examines_only_the_believed_rows(tmp_path):
    """The belief half. Pre-fix: 25,200 (every stored row). Post-fix: 24,000."""
    clean, _ = _build(tmp_path / "clean", N_CORRECTED)
    corr, _ = _build(tmp_path / "corr", N_CORRECTED, CORRECTION_PCT)

    # fixture-is-real: the two configurations must actually differ in the property
    assert _folded_closes(tmp_path / "clean") == (0, True)
    folded, all_current = _folded_closes(tmp_path / "corr")
    assert folded > 0 and not all_current, (
        f"the corrected fixture folded no closes into a node sidecar "
        f"(n_closed_folded={folded}, all_current={all_current}) — this test "
        f"would be comparing a store with itself"
    )
    for a in (clean, corr):
        report = a.verify()
        assert report["healthy"], report["problems"]

    cs, rs = _scan_stats(clean), _scan_stats(corr)
    assert cs["rows"] == rs["rows"] == N_CORRECTED, (
        "both configurations must answer with the same current belief, or their scan "
        f"counts are not comparable: {cs['rows']} vs {rs['rows']}"
    )
    assert rs["rows_examined"] == cs["rows_examined"] == N_CORRECTED, (
        f"the corrected store examined {rs['rows_examined']} rows to return "
        f"{rs['rows']}, against {cs['rows_examined']} for the same store "
        f"uncorrected. Its {folded} superseded rows are still entering the "
        f"row loop only to be skipped, instead of being subtracted from the "
        f"verdict ranges (believed_ranges_closed / close_view)."
    )
    clean.close()
    corr.close()


def test_a_valid_time_window_examines_the_window_not_the_store(tmp_path):
    """The valid-time half. Pre-fix: the whole store. Post-fix: the window.

    `vt_e` is elided on this fixture (every node version is instantaneous),
    so `vt_e > vt_min` collapses to `vt_s >= vt_min` and both ends of the
    window are binary-search cuts on a sorted column.
    """
    a, _ = _build(tmp_path / "s", N_SMALL)
    got = _scan_stats(a, vt_min=0, vt_max=WINDOW_ROWS)
    assert got["rows"] == WINDOW_ROWS, (
        f"the window should select {WINDOW_ROWS} of {N_SMALL}, got {got['rows']}"
    )
    assert got["rows_examined"] == WINDOW_ROWS, (
        f"a {WINDOW_ROWS}-row valid-time window over a {N_SMALL}-row store "
        f"examined {got['rows_examined']} rows. The window is a prefix/suffix "
        f"cut on a vt_s-sorted column; examining the whole store for it is the "
        f"flat sweep this change removed."
    )
    a.close()


def test_a_historical_belief_examines_the_believed_prefix(tmp_path):
    """The transaction-time half, and the one that scales worst: an `as_of`
    deep in history used to sweep the entire store to answer with a slice of
    it. Pre-fix: 64,000 examined for 2,000 returned."""
    a, marks = _build(tmp_path / "s", N_BIG)
    as_of = marks[BELIEVED // ROWS_PER_BATCH - 1]
    got = _scan_stats(a, as_of_tt=as_of)
    assert got["rows"] == BELIEVED, (
        f"the as_of should select {BELIEVED} of {N_BIG}, got {got['rows']}"
    )
    assert got["rows_examined"] == BELIEVED, (
        f"a belief time selecting {BELIEVED} of {N_BIG} node versions examined "
        f"{got['rows_examined']} rows. Rows whose tt_s is still in the future "
        f"at as_of are not in a verdict range at all; examining them is the "
        f"O(store)-for-an-O(answer)-result shape this change removed."
    )
    a.close()


# --- 2. the wall-clock gate ------------------------------------------------ #


def _per_returned_row(small, s_as_of, big, b_as_of, reps: int) -> tuple[float, float]:
    """Median seconds per *returned* row for a historical scan on each configuration.

    Both stores are warmed first — the first scan after opening or compacting
    may build caches (the segment-name map) a cold timing would wrongly
    charge to the scan — and the reps are interleaved, one of each per round,
    so host drift across the two builds lands in both configurations rather than in the
    ratio.
    """
    for a, t in ((small, s_as_of), (big, b_as_of)):
        for _ in range(3):
            a.nodes_columnar(as_of_tt=t)
    times: dict[str, list[float]] = {"small": [], "big": []}
    n: dict[str, int] = {}
    for _ in range(reps):
        for name, a, t in (("small", small, s_as_of), ("big", big, b_as_of)):
            start = time.perf_counter()
            cols = a.nodes_columnar(as_of_tt=t)
            times[name].append(time.perf_counter() - start)
            n[name] = len(cols["uid"])
    return (
        statistics.median(times["small"]) / n["small"],
        statistics.median(times["big"]) / n["big"],
    )


def test_historical_scan_cost_does_not_grow_with_the_store_behind_it(tmp_path):
    small, s_marks = _build(tmp_path / "small", N_SMALL)
    big, b_marks = _build(tmp_path / "big", N_BIG)
    s_as_of = s_marks[BELIEVED // ROWS_PER_BATCH - 1]
    b_as_of = b_marks[BELIEVED // ROWS_PER_BATCH - 1]

    # fixture-is-real, before anything is timed: the two answers must be the
    # same size, or a per-returned-row ratio is measuring answer size
    s_stats = _scan_stats(small, as_of_tt=s_as_of)
    b_stats = _scan_stats(big, as_of_tt=b_as_of)
    assert s_stats["rows"] == b_stats["rows"] == BELIEVED, (
        f"both configurations must return exactly {BELIEVED} rows: "
        f"small={s_stats['rows']}, big={b_stats['rows']}"
    )
    for a in (small, big):
        report = a.verify()
        assert report["healthy"], report["problems"]
    # ... and the stores really are 8x apart, which is the whole axis
    assert big.stats()["n_node_versions"] == N_BIG
    assert small.stats()["n_node_versions"] == N_SMALL

    s_per_row, b_per_row = _per_returned_row(small, s_as_of, big, b_as_of, REPS)
    ratio = b_per_row / s_per_row

    assert ratio < MAX_SCALING_RATIO, (
        f"a historical node scan returning {BELIEVED} rows cost {ratio:.2f}x "
        f"more per returned row on a {N_BIG}-row store than on an "
        f"{N_SMALL}-row one (limit {MAX_SCALING_RATIO}x): "
        f"{s_per_row * 1e6:.4f}us/row -> {b_per_row * 1e6:.4f}us/row. Both "
        f"answers are the same size, so this is the scan paying for rows it "
        f"does not return — the O(store) sweep that the verdict ranges "
        f"replaced. rows_examined: small={s_stats['rows_examined']}, "
        f"big={b_stats['rows_examined']} (both should equal {BELIEVED})."
    )
    assert b_per_row * 1e6 < MAX_ABS_US_PER_ROW, (
        f"the historical scan on the {N_BIG}-row store cost "
        f"{b_per_row * 1e6:.4f}us/row, over the {MAX_ABS_US_PER_ROW}us/row "
        f"bound measured against the fixed engine's 0.329-0.483us/row across "
        f"darwin/arm64 and the development machine"
    )
    small.close()
    big.close()


# --- 3. the answer is unchanged -------------------------------------------- #


def test_the_narrowed_scan_answers_exactly_what_the_sweep_answered(tmp_path):
    """The narrowing must be invisible in the answer, which is the whole
    claim. Cross-checked against `all_node_versions()`, the listing the scan
    used to be a filter over, at several belief times and windows."""
    from gnomon.core.model import OPEN_END, clamp_tt

    a, marks = _build(tmp_path / "s", N_SMALL, CORRECTION_PCT)
    folded, _ = _folded_closes(tmp_path / "s")
    assert folded > 0, "the fixture must carry closes for this to mean anything"

    stored = a.all_node_versions()
    for as_of in (marks[2], marks[8], marks[-1], clamp_tt(OPEN_END)):
        for vt_min, vt_max in ((None, None), (0, WINDOW_ROWS), (N_SMALL // 2, None)):
            want = sorted(
                (v.vid for v in stored
                 if v.tt_s <= as_of < v.tt_e
                 and (vt_min is None or v.vt_e > vt_min)
                 and (vt_max is None or v.vt_s < vt_max)),
            )
            got = sorted(
                a.nodes_columnar(as_of_tt=as_of, vt_min=vt_min, vt_max=vt_max)["vid"]
            )
            ctx = f"as_of={as_of} vt=[{vt_min},{vt_max})"
            assert got == want, (
                f"{ctx}: the narrowed scan returned {len(got)} versions where "
                f"the full listing filtered to {len(want)}"
            )
    a.close()
