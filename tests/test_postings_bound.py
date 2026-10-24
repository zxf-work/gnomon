"""The postings memory gate: a build's identity postings stop growing with the
store once `GNOMON_POSTINGS_BYTES` bounds them.

**The defect this guards.** `read.rs::index_segments` used to fold *every*
not-yet-indexed segment into three store-wide maps — `by_identity`, `by_vid`
and `open_rows` — on *any* point read, and nothing but gc ever released them.
A build that performs even a handful of corrections therefore indexed the
whole store the moment the first one landed. A 30M build shows it exactly:
RSS flat at 1.2–1.8 GB through ~750 s of ingest, then a persistent step to
15.73 GB that never comes back down. 4.85 GB at 10M and 15.73 GB at 30M is
linear in row count, so ≈ 500 GB at 1B — an OOM on a 515 GB node.

The fix is two changes. The first deleted `by_vid` outright: `locate_vid`
bounds the candidate segments by the manifest's `key_lo`/`key_hi` for the
row's `(vt_s, vid)` and binary-searches the survivors, holding nothing
(`store.rs::locate_vid_range_prune_agrees_with_the_unpruned_search`). The
second made what remains per-segment and byte-budgeted, evicted least-recently-
used exactly as the segment cache is, with a probe folding whatever is not
resident on the spot — so correctness never depends on residency.

**What this file measures, and why the primary signal is not RSS.** The
representation collapsed along with the bound: a segment's postings are now a
sorted `(key, row)` pair plus one bit per row, 12 B/row, against ~150 B/row
per map for a `HashMap` slot and a minimum-capacity `Vec` per identity. At
the sizes a CI gate can build, 12 B/row is a *small* share of a build's
resident set, and a peak-RSS ratio across a size step would be measuring the
segments, the dictionary and the allocator far more than the postings. So the
primary claim here is made with the engine's own exact counter —
`postings_index_stats()["resident_bytes"]` — and the RSS ratio is the
secondary, looser check. That is the same reasoning, and the same conclusion,
that `tests/test_incremental_compaction_memory.py` reached when it moved its
fixture-reality check from a peak-RSS ratio to `rows_rewritten`: "the right
claim and the wrong instrument".

**Four mechanisms keep this from being flaky, mirroring
`tests/test_compacted_layout_scaling.py`:**

1. **A fixture-reality control.** The unbounded configuration runs in every
   invocation and must show the postings growing essentially proportionally
   with the store across the size step. A gate whose control has never
   reproduced the defect is an assertion about nothing — and here the control
   *is* the defect's remaining shape, since unbounded postings are still
   O(rows).
2. **A ratio between two sizes measured the same way, on the same host, in
   the same run.** No threshold below is a number remembered from another
   machine.
3. **A subprocess per configuration, with the peak read through the
   `clear_refs`+`VmHWM` (Linux) or `ru_maxrss` (macOS) probe of
   `tests/test_incremental_compaction_memory.py`,** baselined after imports.
   A peak high-water mark never falls, so two configurations in one process would have
   the second inherit the first's. Where neither mechanism can measure this
   section's peak the test **skips with that reason**, and a zero delta is an
   error rather than a pass.
4. **Deterministic cross-checks beside the ratio**: the bounded configuration's
   resident bytes must be *under its budget* at both sizes, its eviction and
   re-fold counters must be non-zero (so the bound is proven to have fired
   rather than merely not been reached), and the two configurations must agree on a
   digest of every version's `(vid, tt_e)` — a bound that dropped rows would
   otherwise pass every memory check in this file.

**The fixture is the campaign's shape**: a bulk `ingest_events` load followed
by a *fixed*, small number of corrections, independent of `n`. That matters
for more than realism. An identity is a hash, so no manifest range can prune
an identity lookup and every such read consults every live segment; under a
*binding* budget each one re-folds whatever was evicted, i.e. O(store) per
read. The engine's default budget is sized so that never happens below
~1.4 × 10⁹ rows (`defaults::POSTINGS_MAX_BYTES`), and this file's bounded configuration
binds deliberately — with few enough identity reads that the re-folds cost
seconds rather than the hours a correction-per-row fixture would.

Native engine only, whatever GNOMON_TEST_BACKEND says: `GNOMON_POSTINGS_BYTES` is
a native-engine knob and a DuckDB run would pass vacuously.

**The identity filter is pinned OFF for the gate above, and ON for one
guard below.** The whole fixture rests on "an identity read consults every
live segment", which the per-segment identity filter deliberately ends: a filter
negative skips a segment without folding it, so a correction folds the one
segment that holds its identity. With the filter on, the unbounded control
measured **1.00x** across the 4x size step (242,560 B at both sizes) and the
262,144-byte budget never bound -- so the fixture-reality control and the
"budget actually fired" check both failed, not because the bound
broke but because the thing it bounds stopped growing. The bound is still
real on the unfiltered path (a store built with `GNOMON_VIDFILTER=0`, or one
whose filters are missing or unreadable), so the four-configuration gate runs there.
`test_with_the_filter_unbounded_postings_stay_flat_across_the_size_step`
then turns the finding into a guard of its own: with the filter on and *no*
budget, the postings must stay flat across the same step.

**The corrections read the whole valid-time line.** The write
path's windowed point read now skips every segment whose valid-time extent
misses the window it reads (the manifest's `vt_min`/`vt_e_max`, as scan
pruning does): an identity is still a hash no manifest range can prune, but
a correction's *window* can be. With each correction reading its own one-tick
window the read folded only the segments that tick spans, and the unbounded
control measured **1.00x** (242,560 B at both sizes) -- the identity filter's
finding a second time. So each correction now states its new props
over `[0, OPEN)`: it still supersedes exactly the one event version, writes
one row, and keeps `rows == n + CORRECTIONS`, but its read spans every
segment, which is the shape this gate needs. Pinning `GNOMON_SEGMENT_PRUNE=0`
instead was tried and rejected: it also unprunes the probe's own scans, whose
mapped segment pages then grow the peak-RSS check with the store (48 MB flat
with pruning, 59 -> 103 MB without, bounded configuration, 100k -> 400k).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest


# --------------------------------------------------------------------------- #
# thresholds, with their provenance
# --------------------------------------------------------------------------- #
#
# Measured on a development laptop (macOS, native engine, this file's own
# probe), N_SMALL = 100,000 -> N_BIG = 400,000, a 4x
# size step, `BOUNDED_BYTES` = 256 KiB:
#
#   configuration          postings bytes           peak RSS delta        wall
#                100k       400k   ratio  100k    400k  ratio
#   unbounded  1,212,980  4,851,380  4.00  14.7MB 27.9MB 1.90   2.9s / 10.8s
#   bounded      242,620    242,560  1.00  13.8MB 23.7MB 1.72   3.3s / 12.4s
#
# The postings-byte ratios are the signal this file exists to state: 4.00x
# against a 4.00x size step is *exactly* proportional, and 1.00x is exactly
# flat. Neither is a timing, so neither moves with host or load; the only way
# they change is if the engine changes.

#: The bounded configuration's postings may grow by at most this factor across a 4x
#: size step. 1.00x measured. 1.5 leaves room for the entry-granularity
#: wobble a byte budget has by construction (the eviction unit is a whole
#: segment, so the resident total lands just under the budget rather than on
#: it, and where it lands depends on the last segment's width), while still
#: being 2.7x below the 4.0 proportional line.
MAX_BOUNDED_GROWTH = 1.5

#: The unbounded control must grow at least this much, or the fixture is not
#: reproducing the shape the bound removes and every threshold here is
#: vacuous. 4.00x measured against a 4x step; 3.0 tolerates a
#: fixture whose per-row posting count shifts slightly with segment layout.
MIN_CONTROL_GROWTH = 3.0

#: Secondary, looser: the bounded configuration's *peak RSS* across the same step. 1.72x
#: measured. 3.0 sits 1.7x above it and still a clear distance below the 4.0
#: a term that grew with the store would produce. See the module docstring
#: for why this is the secondary signal and not the primary one.
MAX_BOUNDED_RSS_GROWTH = 3.0

N_SMALL = 100_000
N_BIG = 400_000
#: Rows per `ingest_events` batch. Large enough that the build is a bulk load
#: rather than a commit benchmark, small enough to seal many segments -- the
#: fixture needs several so eviction has something to choose between.
ROWS_PER_BATCH = 2_000
#: Corrections after the bulk load, **fixed, not scaled with n**: this is the
#: campaign's shape (`scripts/build_synth_store.py::_phase_ranges` runs a few
#: hundred at every scale) and it is what keeps the bounded configuration's O(store)
#: re-folds affordable. See the module docstring.
CORRECTIONS = 10
#: The bounded configuration's budget. 256 KiB holds ~20,000 rows of postings, so it
#: binds at both sizes and by a wider margin at the larger one -- which is
#: the point: a bound that only binds at N_BIG could be satisfied by a
#: fixture that simply stopped growing.
BOUNDED_BYTES = 256 * 1024

#: The probe, written to a temp file and run as a subprocess.
#:
#: `root` arrives as **argv**, not through `str.format`, so nothing in this
#: source has to escape a brace.
_PROBE = r"""
# Build one configuration and report its peak RSS, its postings receipt and a digest of
# what the store answers. Run as a subprocess by tests/test_postings_bound.py,
# never imported.
import json, resource, sys, tempfile
from hashlib import sha256
from pathlib import Path

ROOT = sys.argv[1]
N_ROWS, ROWS_PER_BATCH, CORRECTIONS = (int(a) for a in sys.argv[2:5])
sys.path.insert(0, ROOT)
from gnomon.storage.native import NativeAdapter


def vmhwm_kb():
    # Linux's peak resident set, which clear_refs can reset. None elsewhere.
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1])
    except OSError:
        pass
    return None


def ru_maxrss_kb():
    # The process's *lifetime* peak. Bytes on macOS, kibibytes on Linux.
    ru = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return ru // 1024 if sys.platform == "darwin" else ru


def reset_peak():
    # Reset VmHWM to the current VmRSS (proc(5): write 5 to clear_refs).
    try:
        with open("/proc/self/clear_refs", "w") as f:
            f.write("5")
    except OSError:
        return False
    return True


if vmhwm_kb() is not None and reset_peak():
    MECHANISM, read_peak = "clear_refs+VmHWM", vmhwm_kb
elif sys.platform == "darwin":
    MECHANISM, read_peak = "ru_maxrss", ru_maxrss_kb
else:
    MECHANISM, read_peak = "unavailable", ru_maxrss_kb


def _src(v):
    return "n%d" % (v % 1000)


def _dst(v):
    return "n%d" % ((v * 7) % 1000)


def bulk(vts):
    # the bulk shape a scale build uses: one batch, no supersession read
    return [{"op": "ingest_events", "events": [
        {"src": _src(v), "dst": _dst(v), "rel_type": "R", "disc": "#%d" % v,
         "props": {"v": v, "pad": "x" * 64}, "vt_s": v, "vt_e": v + 1}
        for v in vts]}]


OPEN = 2 ** 62  # the store's open end (gnomon.core.model.OPEN_END)


def correction(v):
    # a correction reads the identity's open versions and then closes them:
    # the read is what folds segments, the close is what the range prune
    # made hold nothing
    return [{"op": "correct",
             "ref": {"kind": "edge", "src": _src(v), "dst": _dst(v),
                     "rel_type": "R", "disc": "#%d" % v},
             "props": {"v": v, "pad": "y" * 64}, "vt_s": 0, "vt_e": OPEN}]


def main():
    base = read_peak()
    with tempfile.TemporaryDirectory() as td:
        a = NativeAdapter(Path(td) / "s")
        a.paranoid = False  # O(versions) per op, and not what is under test
        tt = 1000
        for lo in range(0, N_ROWS, ROWS_PER_BATCH):
            tt += 1
            a.begin()
            a.apply_ops(bulk(range(lo, min(lo + ROWS_PER_BATCH, N_ROWS))), tt)
            a.commit()
        step = max(1, N_ROWS // CORRECTIONS)
        for k in range(CORRECTIONS):
            tt += 1
            a.begin()
            a.apply_ops(correction(k * step), tt)
            a.commit()
        edge = a.postings_index_stats("edge")
        node = a.postings_index_stats("node")
        # the peak *at finalisation*: after every commit and correction, and
        # before materialising the whole store into Python for the digest,
        # which is its own O(store) holder and not what is measured
        peak = read_peak()
        versions = a.all_edge_versions()
        h = sha256()
        for line in sorted("%s|%d" % (v.vid, v.tt_e) for v in versions):
            h.update(line.encode())
        rows = len(versions)
        a.close()
    print(json.dumps({
        "n": N_ROWS, "mechanism": MECHANISM, "base_kb": base, "peak_kb": peak,
        "delta_kb": peak - base, "rows": rows, "digest": h.hexdigest(),
        "edge": edge, "node": node,
        "postings_bytes": edge["resident_bytes"] + node["resident_bytes"],
    }))


main()
"""


@pytest.fixture(scope="module")
def probe(tmp_path_factory) -> Path:
    pytest.importorskip(
        "gnomon.storage.native",
        reason="GNOMON_POSTINGS_BYTES bounds a native-engine structure",
    )
    p = tmp_path_factory.mktemp("probe") / "postings_probe.py"
    p.write_text(_PROBE)
    return p


def _measure(probe: Path, arm: str, n_rows: int, *, vidfilter: bool = False) -> dict:
    """One configuration, one size, in its own process.

    The budget is set through **`GNOMON_POSTINGS_BYTES`** rather than through
    `set_postings_budget()`, so this gate exercises the environment knob an
    operator actually turns -- a handle-level override could pass while the
    env var was mis-parsed or never read.

    `vidfilter=False` (the default, and every configuration of the four-configuration gate) sets
    `GNOMON_VIDFILTER=0`, which disables both the seal-time filter build and
    the lazy rebuild inside a fold -- deleting filter files alone would not
    do it, because the lazy step writes one during the first fold.
    """
    root = str(Path(__file__).resolve().parents[1])
    env = dict(os.environ)
    env["GNOMON_POSTINGS_BYTES"] = "0" if arm == "unbounded" else str(BOUNDED_BYTES)
    if vidfilter:
        env.pop("GNOMON_VIDFILTER", None)
    else:
        env["GNOMON_VIDFILTER"] = "0"
    out = subprocess.run(
        [sys.executable, str(probe), root, str(n_rows), str(ROWS_PER_BATCH),
         str(CORRECTIONS)],
        capture_output=True, text=True, timeout=1800, env=env,
    )
    assert out.returncode == 0, (
        f"the {arm} probe at n={n_rows} failed:\n{out.stdout}\n{out.stderr}"
    )
    result = json.loads(out.stdout.strip().splitlines()[-1])
    result["arm"] = arm
    assert result["rows"] == n_rows + CORRECTIONS, (
        f"the probe lost or invented rows: {result['rows']} against "
        f"{n_rows} + {CORRECTIONS} corrections"
    )
    expected = None if arm == "unbounded" else BOUNDED_BYTES
    assert result["edge"]["budget"] == expected, (
        f"GNOMON_POSTINGS_BYTES={env['GNOMON_POSTINGS_BYTES']} resolved to "
        f"{result['edge']['budget']}, not {expected} -- the env knob this "
        f"gate turns is not the one the engine read"
    )
    if result["mechanism"] == "unavailable":
        pytest.skip(
            "no way to measure this section's peak RSS on this host: "
            "/proc/self/status has VmHWM but /proc/self/clear_refs could not "
            "be written, so the peak cannot be reset, and `ru_maxrss` is a "
            "lifetime high-water mark that the post-import baseline already "
            "exceeds at these sizes. Re-run where clear_refs is writable "
            f"(Linux) or on macOS. Probe said: {result}"
        )
    assert result["delta_kb"] > 0, (
        f"no RSS growth measured at all via {result['mechanism']}: {result}. "
        f"A zero delta means the measurement failed, not that the build used "
        f"no memory."
    )
    return result


@pytest.fixture(scope="module")
def measured(probe) -> dict:
    """All four measurements, once -- each is a subprocess and a real build.
    Unfiltered (`GNOMON_VIDFILTER=0`): see the module docstring."""
    return {
        (arm, n): _measure(probe, arm, n)
        for arm in ("unbounded", "bounded")
        for n in (N_SMALL, N_BIG)
    }


# --- 1. the fixture is real: unbounded postings still grow with the store -- #


def test_the_unbounded_arm_is_really_the_defect_here(measured):
    """The gate must see the shape it guards, on this machine.

    Measured with the engine's own `resident_bytes` rather than with peak
    RSS, for the reason `test_incremental_compaction_memory.py` gives at
    length: a peak-RSS ratio for the same configuration and the same code varied
    8.46x / 4.75x / 4.75x / 4.26x across three hosts and four runs, while an
    exact counter does not move.
    """
    small = measured[("unbounded", N_SMALL)]
    big = measured[("unbounded", N_BIG)]
    growth = big["postings_bytes"] / small["postings_bytes"]
    size_step = N_BIG / N_SMALL

    assert growth >= MIN_CONTROL_GROWTH, (
        f"unbounded identity postings grew only {growth:.2f}x "
        f"({small['postings_bytes']:,} B -> {big['postings_bytes']:,} B) "
        f"across a {size_step:.0f}x size step, under the "
        f"{MIN_CONTROL_GROWTH}x this fixture must reproduce (4.00x when the "
        f"bound was set). Either the corrections stopped folding segments or "
        f"the postings stopped being O(rows) — in both cases every bound in "
        f"this file is vacuous."
    )
    # and they must have been folded by the *reads*, over the whole store:
    # a fold count that never reached the row count would mean the fixture's
    # corrections are not touching the store the way a build's do
    assert big["edge"]["rows_folded"] >= N_BIG, (
        f"the unbounded configuration folded only {big['edge']['rows_folded']:,} rows "
        f"of a {N_BIG:,}-row store: the correction's identity read is not "
        f"reaching every segment, so this fixture is not the 30M build's shape"
    )


# --- 2. the bound holds, and holds flat across the size step -------------- #


def test_bounded_postings_do_not_grow_with_the_store(measured):
    small = measured[("bounded", N_SMALL)]
    big = measured[("bounded", N_BIG)]
    growth = big["postings_bytes"] / small["postings_bytes"]

    assert growth <= MAX_BOUNDED_GROWTH, (
        f"identity postings under a {BOUNDED_BYTES:,}-byte budget grew "
        f"{growth:.2f}x ({small['postings_bytes']:,} B -> "
        f"{big['postings_bytes']:,} B) across a {N_BIG / N_SMALL:.0f}x size "
        f"step, over the {MAX_BOUNDED_GROWTH}x bound (1.00x when it was set). "
        f"The budget is not bounding what it claims to."
    )
    for arm in (small, big):
        for kind in ("edge", "node"):
            st = arm[kind]
            assert st["resident_bytes"] <= BOUNDED_BYTES, (
                f"{kind} postings held {st['resident_bytes']:,} B at "
                f"n={arm['n']:,} against a {BOUNDED_BYTES:,} B budget"
            )


def test_the_budget_actually_fired_rather_than_simply_not_being_reached(measured):
    """A bound that never binds proves nothing. Both bounded configurations must show
    evictions *and* re-folds: segments dropped to stay under the budget, and
    later probes folding them again — the round trip that makes
    "correctness never depends on residency" a measurement."""
    for n in (N_SMALL, N_BIG):
        st = measured[("bounded", n)]["edge"]
        assert st["evictions"] > 0 and st["rebuilds"] > 0, (
            f"at n={n:,} the bounded configuration recorded {st['evictions']} evictions "
            f"and {st['rebuilds']} re-folds: the {BOUNDED_BYTES:,}-byte budget "
            f"never bound, so nothing below is tested"
        )
        unbounded = measured[("unbounded", n)]["edge"]
        assert unbounded["evictions"] == 0 and unbounded["rebuilds"] == 0, (
            f"at n={n:,} the *unbounded* configuration evicted {unbounded['evictions']} "
            f"times — GNOMON_POSTINGS_BYTES=0 must mean unbounded"
        )


# --- 3. secondary: peak RSS at finalisation ------------------------------- #


def test_peak_rss_at_finalisation_stays_sublinear_under_a_bounded_budget(measured):
    """The looser, host-sensitive check. See the module docstring for why the
    postings-byte ratio above is the primary signal: at CI sizes the postings
    are a small share of a build's resident set, so this bounds the whole
    build rather than isolating the term under test."""
    small = measured[("bounded", N_SMALL)]
    big = measured[("bounded", N_BIG)]
    growth = big["delta_kb"] / small["delta_kb"]
    assert growth <= MAX_BOUNDED_RSS_GROWTH, (
        f"peak RSS at finalisation grew {growth:.2f}x "
        f"({small['delta_kb'] / 1024:.0f} MB -> {big['delta_kb'] / 1024:.0f} "
        f"MB) across a {N_BIG / N_SMALL:.0f}x size step under a "
        f"{BOUNDED_BYTES:,}-byte postings budget, over the "
        f"{MAX_BOUNDED_RSS_GROWTH}x bound (1.72x when it was set), measured "
        f"via {big['mechanism']}"
    )


# --- 4. the bound changes cost, never answers ----------------------------- #


def test_the_bounded_and_unbounded_arms_answer_identically(measured):
    """The check that makes every memory bound above worth having. A budget
    that dropped rows, or that lost a close, would satisfy all of them."""
    for n in (N_SMALL, N_BIG):
        bounded = measured[("bounded", n)]
        unbounded = measured[("unbounded", n)]
        assert bounded["rows"] == unbounded["rows"], (
            f"at n={n:,} the bounded configuration returned {bounded['rows']:,} versions "
            f"against the unbounded configuration's {unbounded['rows']:,}"
        )
        assert bounded["digest"] == unbounded["digest"], (
            f"at n={n:,} a {BOUNDED_BYTES:,}-byte postings budget changed what "
            f"the store answers: the (vid, tt_e) digest of every edge version "
            f"is {bounded['digest'][:16]}… bounded against "
            f"{unbounded['digest'][:16]}… unbounded. Correctness must never "
            f"depend on residency."
        )


# --- 5. with the filter on, there is nothing left to bound --------------- #


@pytest.fixture(scope="module")
def measured_filtered(probe) -> dict:
    """The unbounded configuration at both sizes with the identity filter **on** (the
    engine default): two more subprocess builds."""
    return {n: _measure(probe, "unbounded", n, vidfilter=True)
            for n in (N_SMALL, N_BIG)}


def test_with_the_filter_unbounded_postings_stay_flat_across_the_size_step(
        measured, measured_filtered):
    """The identity filter's claim, as a guard: with per-segment filters, a
    correction folds the segment that holds its identity rather than every
    live segment, so unbounded postings stop growing with the store.

    Measured 1.00x (242,560 B at both 100k and 400k) when this was added.
    The same `MAX_BOUNDED_GROWTH` a byte budget is held to applies -- the
    filter should do, with no budget at all, what the budget does -- and the
    unfiltered control in `measured` must still show the growth, or this
    would be comparing flat with flat.
    """
    small, big = measured_filtered[N_SMALL], measured_filtered[N_BIG]
    growth = big["postings_bytes"] / small["postings_bytes"]
    assert growth <= MAX_BOUNDED_GROWTH, (
        f"with the identity filter on, unbounded postings grew {growth:.2f}x "
        f"({small['postings_bytes']:,} B -> {big['postings_bytes']:,} B) across "
        f"a {N_BIG / N_SMALL:.0f}x size step, over {MAX_BOUNDED_GROWTH}x "
        f"(1.00x when this guard was added): corrections are folding segments "
        f"the filter should have skipped"
    )
    # the filter must actually have been consulted -- a flat result from a
    # store that simply built no filters would be the wrong reason
    for n, arm in measured_filtered.items():
        assert arm["edge"]["filter_probes"] > 0, (
            f"at n={n:,} the filtered configuration made no filter probes: the store has "
            f"no filters, so a flat postings line proves nothing about them"
        )
        assert arm["edge"]["rows_folded"] < n, (
            f"at n={n:,} the filtered configuration still folded "
            f"{arm['edge']['rows_folded']:,} rows -- the whole store"
        )
    # and it changes cost, never answers
    for n in (N_SMALL, N_BIG):
        assert measured_filtered[n]["digest"] == measured[("unbounded", n)]["digest"], (
            f"at n={n:,} the identity filter changed what the store answers: "
            f"the (vid, tt_e) digest differs from the unfiltered configuration's"
        )
