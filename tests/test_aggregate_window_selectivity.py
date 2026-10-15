"""A windowed `aggregate_events` costs the *window*, not the store.

`aggregate.rs`'s whole-manifest open loop had been classified as a bulk walk, which made it open
every segment `OpenMode::Uncached`. It is not a bulk walk — it is a
per-QUERY path, and the loop ran *before* `ScanSet::select` had a chance to
prune anything, so every aggregate decoded every segment on every call and
threw the columns away again. Measured at 1M rows on a 20-core workstation,
a 10-tick
window over a 1.05M-tick extent went **0.85 ms -> 70.7 ms (83x)** and came
to cost **76 %** of a whole-store aggregate; starving the *old* engine's
segment cache reproduced the new numbers exactly, and the new engine was
indifferent to the cache budget. Segment count was identical at both commits
and is not the mechanism.

The fix is two ordered steps in
`NativeStore::aggregate_edges`:

1. prune on the manifest's own zone maps — `vt_min`/`vt_max`/`vt_e_max` and
   the folded `tt_s` bound `SegmentEntry` already carries — through
   `scan::prunes_bounds`, the same tier-1 predicate `ScanSet::prunes` uses,
   *before* any file is mapped;
2. open the survivors **cached** again, which is safe precisely because (1)
   bounds what the session cache can retain by the window rather than by the
   store. The genuine whole-store walks (`all_edge_versions` /
   `all_node_versions`, `index_segments`, compaction's merge reads) stay
   uncached — `test_suffix_replay.py::
   test_a_bulk_walk_leaves_the_segment_cache_alone` guards that half.

This file gates both halves of the restored behaviour, and it depends on the
fix in the same way `test_compacted_layout_scaling.py` depends on its own:
`segments_opened`/`segments_pruned` exist only on the fixed engine, so a
pre-fix `gnomon._engine` fails at the fixture check rather than passing
vacuously.

**Four things keep this from being flaky.**

1. *A structural precondition, checked before anything is timed.*
   `test_fixture_is_many_segments_and_a_narrow_window_opens_one` asserts the
   store really has >= `MIN_SEGMENTS` live edge-event segments and that the
   10-tick window opens exactly one of them. A gate whose fixture has never
   had the shape it assumes is an assertion about nothing.
2. *A timing-independent companion signal.* The latency gate below asserts
   `segments_opened` alongside the ratio. A loaded host can slow a
   measurement down; it cannot make the engine open 1 segment instead of
   240. The two signals fail together on the real defect and cannot both be
   faked by noise.
3. *Duration-matched, warmed, median-of-batches timing.* Each repetition
   times a whole batch of calls and divides, and the batch is sized from a
   pilot call so that both calls in the ratio are measured over repetitions
   of the same wall duration (`REP_TARGET_MS`). A fixed batch size does not
   do this: with 20 calls per repetition the ~10us narrow call's repetition
   was ~1 ms, short enough that a single scheduler preemption set its
   median, and a run with two test suites in parallel measured 48us for
   it and failed the gate at 0.117 while `segments_opened` was still 1.
   Sizing by duration puts contention into both numbers instead of into
   whichever has the shorter repetition.
4. *Thresholds sited between the measured healthy and defective values,
   with the margin written down* — see the provenance block above
   `MAX_WINDOW_RATIO`.

The engine call (`adapter.aggregate_events_columnar`) is timed rather than
`call_operator(..., "aggregate_events", ...)` deliberately: the plan-layer
envelope adds a flat ~0.19 ms of Python to *every* operator call
(`gnomon/temporal/algebra.py`), which is unrelated to storage and would
dominate — and so hide — a sub-millisecond narrow window. The two timed
calls differ in exactly one argument, the window.

Native engine specifically, whatever `GNOMON_TEST_BACKEND` says: segment
manifests and a segment cache budget have no portable-backend equivalent,
and a DuckDB run would pass every test here vacuously.
"""

from __future__ import annotations

import statistics
import time
from pathlib import Path

import pytest

from gnomon.core.model import OPEN_END

#: Generations, each committing one segment. The campaign's 1M store had 283
#: >= 200 here keeps "opened 1 of many" a two-order-of-magnitude
#: statement. The store is built once for the whole file (`store_root`), in
#: ~15 s.
N_SEGMENTS = 240
MIN_SEGMENTS = 200
#: Rows per generation. Sized by a *measurement*, not by taste: the narrow
#: call has a per-call floor that is independent of the window -- the O(live
#: segments) manifest prune walk, plus the Python/pyo3 boundary -- and the
#: gate is only meaningful when the whole-extent call's segment work is large
#: against that floor. The floor is ~5us on darwin/arm64 but ~47us on a second host
#: (x86, the measurement host), and at 60 rows per generation the whole-extent
#: call cost only ~0.4 ms there, so the ratio was 0.12 on a *correct* engine
#: and the gate failed. At 400 the whole-extent call is 1.28 ms here and
#: 1.77 ms on that host, and the measured ratio is 0.0038 here and 0.0265 there.
ROWS_PER_BATCH = 400

#: Generation `g` owns valid times `[g * ROWS_PER_BATCH, (g+1) * ROWS_PER_BATCH)`
#: -- disjoint, one event per tick, which is the shape of the campaign corpus
#: (`eval_harness._vt_s`: event `i` starts at tick `i`). Overlapping ranges
#: would make every segment intersect every window and the gate vacuous.
T_MAX = N_SEGMENTS * ROWS_PER_BATCH
#: The fixed narrow window: 10 ticks at a fixed absolute offset, the same
#: regression probe used and the same width as the `*.w10` registry
#: queries in `scripts/eval_harness.py`.
FIXED_WINDOW_A = 1_000
FIXED_WINDOW_WIDTH = 10

#: Starved and generous segment-cache budgets. 1 MB cannot hold this store's
#: decoded columns; 32 GB is the campaign's own setting.
CACHE_STARVED = "1000000"
CACHE_GENEROUS = "32000000000"


def _native_adapter(tmp: Path):
    """The native engine, regardless of GNOMON_TEST_BACKEND (see module doc)."""
    native = pytest.importorskip(
        "gnomon.storage.native",
        reason="aggregate window selectivity is a native-engine property",
    )
    a = native.NativeAdapter(tmp)
    a.paranoid = False  # the paranoid disjointness check is O(versions) per op
    return a


def _build_many_segments(tmp: Path):
    """`N_SEGMENTS` generations with disjoint valid-time ranges.

    One `begin`/`apply_ops`/`commit` is one generation is one segment here:
    a batch of `ROWS_PER_BATCH` edges is far below the segment-size target,
    so it never splits, and nothing compacts. Two relation types so the
    `rel_type` grouping dimension has something to group.
    """
    a = _native_adapter(tmp)
    tt = 1_000
    for g in range(N_SEGMENTS):
        tt += 1
        base = g * ROWS_PER_BATCH
        ops = [
            {"op": "assert_edge", "src": f"s{(base + i) % 50}",
             "dst": f"d{(base + i) % 37}",
             "rel_type": "R" if (base + i) % 2 == 0 else "Q",
             "props": {"v": base + i},
             "vt_s": base + i, "vt_e": base + i + 1}
            for i in range(ROWS_PER_BATCH)
        ]
        a.begin()
        a.apply_ops(ops, tt)
        a.commit()
    return a


def _aggregate(adapter, t_a: int, t_b: int) -> dict:
    """`aggregate_events` at the engine boundary, grouped by `rel_type`.

    No `time_bucket` dimension and so no `stride`: the narrow and the whole-
    extent call then differ in *exactly* one argument, the window, and the
    comparison cannot be confounded by a different bucket count.
    """
    return adapter.aggregate_events_columnar(
        OPEN_END, t_a, t_b, None, None,
        [("rel_type", None)], [("count", None)], 100_000)


def _answer(got: dict) -> tuple:
    """The part of an aggregate result that is the answer, as a comparable."""
    return (int(got["rows_total"]), list(got["rel_names"]),
            [list(k) for k in got["keys"]],
            [list(c) for c in got["aggs"]])


#: Wall time each timed repetition should take. The batch size is derived
#: from a pilot call so that *every* call being compared is measured over a
#: repetition of the same duration, whether one call costs 10 microseconds or
#: one costs a millisecond. Without this the cheap call's median is set by
#: whether a scheduler preemption landed inside its ~1 ms repetition -- which
#: is how an early version of this gate measured 48us for a call that costs
#: 12us, on a run with two test suites in parallel, and failed.
REP_TARGET_MS = 20.0
MAX_BATCH = 20_000


def _p50_ms_per_call(fn, *, warmup: int = 3, reps: int = 9) -> float:
    """Median per-call wall time in ms, timed a batch at a time.

    A single warm narrow-window call is ~10 microseconds -- at or below
    `perf_counter`'s useful resolution once loop overhead is counted, and far
    below one scheduler slice -- so each repetition times a whole batch and
    divides, and the batch is sized from a pilot call to make the repetition
    `REP_TARGET_MS` long whatever the call costs. Contention then inflates
    both numbers in the ratio together, instead of landing entirely on
    whichever one has the shorter repetition.

    Warmups are untimed: the first call after an open builds the segment-name
    map and the close index, which belong to neither number being
    compared.
    """
    for _ in range(warmup):
        fn()
    start = time.perf_counter()
    fn()
    pilot_ms = (time.perf_counter() - start) * 1e3
    batch = max(1, min(MAX_BATCH, int(REP_TARGET_MS / max(pilot_ms, 1e-4))))
    per_call = []
    for _ in range(reps):
        start = time.perf_counter()
        for _ in range(batch):
            fn()
        per_call.append((time.perf_counter() - start) * 1e3 / batch)
    return statistics.median(per_call)


# --- 1. the fixture is real, and the prune is structural ------------------- #


@pytest.fixture(scope="module")
def store_root(tmp_path_factory):
    """One 96,000-row, 240-segment store for the whole file.

    Module-scoped because building it costs ~15 s and every test here wants
    the same immutable store; nothing below writes to it, and the two tests
    that vary `GNOMON_SEGMENT_CACHE_BYTES` do so by re-*opening* it, which is
    where the budget is read (`store.rs:465`).
    """
    root = tmp_path_factory.mktemp("aggsel") / "s"
    _build_many_segments(root).close()
    return root


def test_fixture_is_many_segments_and_a_narrow_window_opens_one(store_root):
    adapter = _native_adapter(store_root)
    report = adapter.verify()
    assert report["healthy"], report["problems"]

    segments = report["closes_by_lane"]["edge_event"]["segments"]
    assert segments >= MIN_SEGMENTS, (
        f"the gate is about opening few of *many* segments; this fixture "
        f"produced only {segments} (wanted >= {MIN_SEGMENTS})"
    )
    assert report["rows"] == T_MAX

    narrow = _aggregate(adapter, FIXED_WINDOW_A, FIXED_WINDOW_A + FIXED_WINDOW_WIDTH)
    assert narrow["segments_total"] == segments
    assert narrow["segments_opened"] == 1, (
        f"a {FIXED_WINDOW_WIDTH}-tick window falls inside one generation's disjoint "
        f"valid-time range, so exactly one segment's manifest bounds can "
        f"intersect it; the engine opened "
        f"{narrow['segments_opened']} of {narrow['segments_total']}. "
        f"Opening all of them is the pre-fix behaviour this file gates "
        f"(the open loop ran ahead of every prune)."
    )
    assert narrow["segments_pruned"] + narrow["segments_opened"] == narrow["segments_total"]
    assert narrow["rows_total"] > 0, "a vacuous window would make the gate vacuous"

    whole = _aggregate(adapter, 0, T_MAX)
    assert whole["segments_pruned"] == 0, (
        "a whole-extent window cannot prune anything -- if it does, the "
        "prune is wrong, not selective"
    )
    assert whole["segments_opened"] == segments
    adapter.close()


def test_pruning_does_not_change_the_answer(store_root):
    """Selectivity must be invisible in the answers, at every window and
    under either cache budget (residency never changes answers)."""
    adapter = _native_adapter(store_root)
    windows = [(0, T_MAX), (FIXED_WINDOW_A, FIXED_WINDOW_A + FIXED_WINDOW_WIDTH), (0, ROWS_PER_BATCH),
               (T_MAX - ROWS_PER_BATCH, T_MAX), (FIXED_WINDOW_A, FIXED_WINDOW_A + 1_000)]
    for t_a, t_b in windows:
        got = _aggregate(adapter, t_a, t_b)
        total = sum(int(c) for c in got["aggs"][0])
        # one event per tick, so the window width *is* the answer
        assert total == min(t_b, T_MAX) - max(t_a, 0), (
            f"window [{t_a}, {t_b}) counted {total} events; the fixture puts "
            f"exactly one event on every tick; opened "
            f"{got['segments_opened']} of {got['segments_total']} segments"
        )
    adapter.close()


@pytest.mark.parametrize("budget", [CACHE_STARVED, CACHE_GENEROUS])
def test_answers_are_identical_under_either_cache_budget(store_root, monkeypatch, budget):
    monkeypatch.setenv("GNOMON_SEGMENT_CACHE_BYTES", budget)
    adapter = _native_adapter(store_root)
    narrow = _answer(_aggregate(adapter, FIXED_WINDOW_A, FIXED_WINDOW_A + FIXED_WINDOW_WIDTH))
    whole = _answer(_aggregate(adapter, 0, T_MAX))
    adapter.close()
    assert narrow[0] > 0 and whole[0] > 0
    # the counts a window-bounded answer must have, from the fixture's own
    # arithmetic rather than from a second engine call
    assert sum(int(c) for c in narrow[3][0]) == FIXED_WINDOW_WIDTH
    assert sum(int(c) for c in whole[3][0]) == T_MAX


# --- 2. the performance gate ---------------------------------------------- #
#
# Measured on this machine (darwin/arm64), native engine, this file's own
# fixture (240 segments, 96,000 rows, one event per tick) and this file's own
# statistic:
#
#                                  window-10      whole-extent    ratio
#   post-fix, cache 32 GB          4.9 us         1.28 ms         0.0038
#   post-fix, cache 1 MB           ~5 us          9.70 ms         ~0.0005
#
# and on an x86 workstation (the measurement host), same fixture, under the gate lock
# at load < 3:
#
#   post-fix, cache 32 GB          47.0 us        1.77 ms         0.0265
#   post-fix, cache 1 MB           --             10.03 ms        (5.66x)
#
# and on a second host at 1M rows / 279 segments -- the measurement this gate exists
# because of (plus this file's own three-configuration comparison;
# `aggregate_events` over a 1.05M-tick extent,
# engine-side p50 under the gate lock):
#
#                                  window-10      whole-extent    ratio
#   pre-fix                        71.7 ms        94.0 ms         0.76
#   old engine (cached)            0.103 ms       30.3 ms         0.0034
#   post-fix (this branch)         0.053 ms       21.9 ms         0.0024
#
# The ratio is the primary signal because both numbers come from the same
# process, the same store and the same query shape, so host speed largely
# divides out -- with one caveat this file learned the hard way. The narrow
# call also pays a per-call floor that does *not* scale with the window (the
# O(live segments) manifest walk, plus the Python/pyo3 boundary): ~5us here,
# ~47us on the second host. That floor is why `ROWS_PER_BATCH` is what it is; see its
# note. An earlier revision used 60 rows per generation, which left that host's
# whole-extent call at ~0.4 ms -- only ~8x its own floor -- and a *correct*
# engine measured 0.117-0.128 there against a 0.10 limit, on three
# independent runs including one under the gate lock at load < 3.
#
# MAX_WINDOW_RATIO = 0.15 sits ~5.7x above the worst healthy measurement
# (0.0265, the second host, whose 47us floor is 9.6x this machine's) and ~5x below the
# defective value (0.76). A partial regression has to close most of a 29x
# gap before it slips through, and `segments_opened` (asserted alongside,
# and immune to host speed and load alike) has to go wrong at the same
# time.
MAX_WINDOW_RATIO = 0.15

# The second, independent property, and the one that says the aggregate path
# is back on the session cache at all: on that host the old cached engine's
# whole-extent aggregate went 28.7 ms -> 105.2 ms (3.7x) when the budget was
# cut to 1 MB, and this branch's goes 21.9 -> 96.0 ms (4.4x), while the
# uncached pre-fix engine did not move at all (91.8 -> 90.3 ms, 0.98x -- it
# never consulted the cache). This fixture measures 7.6x here and 5.66x on
# that host. MIN_CACHE_RATIO = 2.0 sits ~2.8x below the weakest healthy
# measurement and ~2x above the defective one, which is pinned at 1.0 by the mechanism rather than by
# noise: a path that does not read the budget cannot respond to it.
MIN_CACHE_RATIO = 2.0


def _whole_extent_p50(tmp: Path, monkeypatch, budget: str) -> tuple[float, dict]:
    monkeypatch.setenv("GNOMON_SEGMENT_CACHE_BYTES", budget)
    adapter = _native_adapter(tmp)
    got = _aggregate(adapter, 0, T_MAX)
    p50 = _p50_ms_per_call(lambda: _aggregate(adapter, 0, T_MAX))
    adapter.close()
    return p50, got


def test_a_ten_tick_window_costs_a_fraction_of_the_whole_extent(store_root, monkeypatch):
    monkeypatch.setenv("GNOMON_SEGMENT_CACHE_BYTES", CACHE_GENEROUS)
    adapter = _native_adapter(store_root)

    narrow_stats = _aggregate(adapter, FIXED_WINDOW_A, FIXED_WINDOW_A + FIXED_WINDOW_WIDTH)
    whole_stats = _aggregate(adapter, 0, T_MAX)
    narrow = _p50_ms_per_call(lambda: _aggregate(adapter, FIXED_WINDOW_A, FIXED_WINDOW_A + FIXED_WINDOW_WIDTH))
    whole = _p50_ms_per_call(lambda: _aggregate(adapter, 0, T_MAX))
    adapter.close()

    ratio = narrow / whole
    assert ratio <= MAX_WINDOW_RATIO, (
        f"a {FIXED_WINDOW_WIDTH}-tick window cost {ratio:.3f} of the whole "
        f"{T_MAX}-tick extent (limit {MAX_WINDOW_RATIO}): "
        f"{narrow * 1e3:.1f}us vs {whole * 1e3:.1f}us over "
        f"{whole_stats['segments_total']} segments. The window has lost its "
        f"selectivity -- the aggregate is paying for the whole store on "
        f"every call, which is the regression (measured "
        f"0.76 at 1M) that prune-before-open and the cached open were "
        f"written to remove."
    )
    # timing-independent companion (mechanism 2 in the module doc)
    assert narrow_stats["segments_opened"] == 1, (
        f"the ratio passed but the engine still opened "
        f"{narrow_stats['segments_opened']} of "
        f"{narrow_stats['segments_total']} segments for a "
        f"{FIXED_WINDOW_WIDTH}-tick window"
    )
    assert whole_stats["segments_opened"] == whole_stats["segments_total"]


def test_the_whole_extent_aggregate_is_cache_sensitive_again(store_root, monkeypatch):
    """The other half of the fix: survivors open **cached**.

    An aggregate that opens its segments uncached is by construction
    indifferent to `GNOMON_SEGMENT_CACHE_BYTES` -- that indifference is how
    the audit identified the regression, and it is what this asserts
    is gone. A whole-extent aggregate opens every segment, so a budget too
    small to hold them must cost visibly more than one that is not.
    """
    generous, gen_stats = _whole_extent_p50(store_root, monkeypatch, CACHE_GENEROUS)
    starved, starved_stats = _whole_extent_p50(store_root, monkeypatch, CACHE_STARVED)

    assert gen_stats["segments_opened"] == starved_stats["segments_opened"], (
        "the cache budget must not change which segments are opened"
    )
    assert _answer(gen_stats) == _answer(starved_stats), (
        "residency must not change answers"
    )

    ratio = starved / generous
    assert ratio >= MIN_CACHE_RATIO, (
        f"a whole-extent aggregate cost {starved:.3f}ms at a "
        f"{CACHE_STARVED}-byte segment-cache budget and {generous:.3f}ms at "
        f"{CACHE_GENEROUS} -- only {ratio:.2f}x, under the "
        f"{MIN_CACHE_RATIO}x floor. A path that does not respond to the "
        f"budget is not using the session cache at all, which is exactly "
        f"the `OpenMode::Uncached` aggregate open that was reverted (the "
        f"pre-fix engine measured 0.98x, the cached engine 3.7x)."
    )
