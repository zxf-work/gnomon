"""The memory-accounting instrument: `memory_accounting()` names where a build's resident
bytes went, and costs nothing to read.

38–56 GB band. `postings_peak_bytes` — the only per-component receipt the
engine had — accounted for 12.25 GB of that, and `segment_cache_stats`
reported one accounted total that silently mixes two quantities with
different behaviour. Everything else was a subtraction. This accessor turns
the subtraction into a reading: one line per long-lived structure, each with
its own scaling law, so a measurement can *rule components out* rather than
leave a residual labelled "other".

**The two identities worth asserting exactly.** Most of what this file checks
is arithmetic the engine guarantees, not a threshold:

* the budget charges decoded heap and never the mapping:
  `segment_cache_accounted == segment_cache_decoded + segment_cache_header`,
  with `segment_cache_mapped` excluded outright. The mapping is the store
  itself — file-backed pages the kernel manages — and charging it was what
  made a 32 GB budget produce 52.6 GB of residency at 1B.
* the three classes partition what is named:
  `anonymous_total` is every heap component summed, `file_mapped_total` is
  the store's mapped bytes, and they are read against `RssAnon` and
  `RssFile + RssShmem` respectively.
* `pinned_mapped >= segment_cache_mapped`, whenever pinning is on. Pinning maps
  *every* segment of the open generation, so a cached segment's mapping is
  always also a pinned one — which is what makes the budget's credit for
  evicting a segment's `mapped` share a fiction on a pinned store.

Neither is a tolerance. A change that breaks either has changed what the
budget means.

**The sum-against-RSS check, and why its fraction is loose.** The accounting
estimates *allocated capacity* — `Vec`/`String`/`HashMap` capacities and the
bytes they point at. It cannot see allocator slack, arena retention, the
Python interpreter, numpy's buffers, or the pages of a mapping the kernel has
not faulted in. So the honest claim is one-sided and wide: the accounted
total must explain a stated minimum share of the resident growth a store
causes, and must not exceed it by more than a stated factor. `MIN_EXPLAINED`
and `MAX_EXPLAINED` below record the measured values and the margins chosen
around them. This is the secondary signal; the identities above are the
primary one, for exactly the reason `tests/test_postings_bound.py` states at
length about its own RSS ratio — "the right claim and the wrong instrument".

**The cost check.** The accessor is meant to be sampled at a progress line
and at every point op of a build's post-bulk phases, so its cost has to be
invisible against the work it observes. The gate times the accessor
*in-process*, inside the configuration that pays for it: at this fixture's size the
whole build is ~2.3 s and two interpreters differ by tens of milliseconds
for reasons of their own, which is the same order as the quantity under
test, so a difference of two walls is the wrong instrument for a
half-percent claim. The cross-process comparison is kept as a much looser
companion (`MAX_ARM_CPU_OVERHEAD`): process CPU time over interleaved pairs of
runs, which catches a hook or an accessor costing a sizable share of the
build, and is shown to (`test_the_paired_gate_fails_when_sampling_costs_something`).
"""

from __future__ import annotations

import json
import os
import statistics
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

ROOT = str(Path(__file__).resolve().parents[1])

#: Components that are anonymous heap, and so belong against `RssAnon`.
#: `segment_cache_mapped`/`pinned_mapped` are file-backed; the `*_entries`
#: and `*_segments` lines are counts, not bytes; `segment_cache_accounted`
#: is the sum of two lines already counted and would double-count.
ANON = (
    "segment_cache_decoded", "segment_cache_header", "postings", "tcsr",
    "name_index_cache", "close_index", "staging", "manifest", "dictionary",
    "stats_accum", "verified_set",
    # Identity-filter bytes left on the heap -- only a filter whose
    # write failed. The mapped ones are file-backed (`vid_filter_mapped`).
    "vid_filter_heap", "node_belief",
)

COUNTS = ("segment_cache_entries", "segment_cache_evictions",
          "segment_cache_rows", "pinned_segments", "name_index_entries",
          "vid_filter_segments")

#: Every key the accessor promises. A component that disappears is a receipt
#: that silently stops attributing something.
EXPECTED = set(ANON) | set(COUNTS) | {"segment_cache_accounted",
                                      "segment_cache_budget",
                                      "segment_cache_mapped", "pinned_mapped",
                                      "vid_filter_mapped",
                                      "anonymous_total", "file_mapped_total"}

#: Measured on the fixture below (macOS, 60k rows + 40 corrections): the
#: accounted total came to **0.578** of the store's resident growth
#: (11,099,156 B against 19,218,432 B), the remainder being allocator slack,
#: the Python objects the ingest path builds and the interpreter's own
#: growth. 0.30 leaves better than 1.9x of room downward while still failing
#: loudly if a future change adds a store-sized structure the accessor does
#: not name — which is the regression this number exists to catch.
MIN_EXPLAINED = 0.30

#: The other side, and a ceiling rather than a measured value. The accounting
#: counts capacity, and a `Vec` reserved but not written is capacity the
#: kernel has not backed with pages, so the total *can* legitimately exceed
#: the growth it explains — on a host where the baseline is taken later, or
#: where a mapping is counted but never faulted in. 2.5 is where "the
#: accessor is inventing bytes" becomes the better reading.
MAX_EXPLAINED = 2.5

#: `memory_accounting()` sampled on the build's own cadence must cost less
#: than this share of wall. **Measured at 0.00127** — 70 samples costing
#: 2.88 ms of a 2.276 s build, i.e. ~41 us per call. The call is O(cached
#: segments + pinned segments + staged rows), and the cadence is a few
#: hundred calls per build at any scale. 0.005 is the stated gate, 3.9x above
#: what was measured.
MAX_WALL_FRACTION = 0.005

#: The cross-process companion gate: the sampling run's **CPU time** over the
#: non-sampling run's, as the median of ARM_PAIRS interleaved pairs, may not
#: exceed this.
#:
#: Why CPU time and pairs. The gate was a single wall-clock
#: difference held to 5 %, and it failed 6 times in 12 isolated re-runs on
#: the development host under load 2-4, at commits that cannot differ in
#: what it measures (both runs use the same build). Wall is the wrong
#: instrument here: this fixture's wall is ~2.3 s of which only ~0.7 s is CPU
#: -- the rest is commit fsyncs -- and the sampling it bounds is pure CPU, so
#: wall adds the volume's latency spread to a CPU quantity. And one pair is
#: the wrong design: a host whose load drifts during the two runs moves one
#: side and not the other. Interleaving A B A B ... and taking the median of
#: the per-pair ratios cancels drift slower than one pair and ignores the
#: pairs a burst of contention landed in.
#:
#: The band, from the measured spread: nine repetitions of the 7-pair
#: statistic with no added cost, on a laptop at load average ~4, gave
#: -5.3 %, -2.1 %, -0.7 %, -0.35 %, +0.5 %, +0.9 %, +2.0 %, +2.7 % and +6.2 %
#: (standard deviation 3.2 %, worst 6.2 %). 12 % is about 3.7 standard
#: deviations and twice the worst value seen. What the
#: gate exists to catch is an accessor or hook that costs on the order of the
#: build: the in-process gate above already pins the accessor itself at
#: ~0.2-0.4 % of CPU, and `test_the_paired_gate_fails_when_sampling_costs_something`
#: shows that an added cost of ~50 % of the build's CPU is caught (and 4 ms
#: per sample, +23 %, was caught too).
MAX_ARM_CPU_OVERHEAD = 0.12

#: Interleaved pairs for the companion gate (the median of 7 ratios; fewer
#: and one contended pair moves the median).
ARM_PAIRS = 7

#: The cost the self-test adds to every sampled `memory_accounting()` call.
#: Measured on the laptop (70 samples, ~0.86 s of build CPU): 4 ms per call
#: moved the 7-pair statistic by +22.9 %, 8 ms by +48.8 % -- the latter is
#: four times MAX_ARM_CPU_OVERHEAD, so the self-test asserts a trip with room.
INJECTED_COST_US = 8_000

N_ROWS = 60_000
ROWS_PER_BATCH = 2_000
#: Fixed, not scaled: the campaign's own shape
#: (`scripts/build_synth_store.py::_phase_ranges` runs a few hundred point
#: ops at every scale). These are what populate the segment cache and the
#: postings, because each resolves an identity against the committed store —
#: the ingest path alone reads nothing back and leaves both empty.
CORRECTIONS = 40

#: The configuration runs with an explicit, *binding* segment-cache budget rather than
#: the host default. Two reasons. The default is unbounded on any host whose
#: RAM the engine cannot detect (`detected_ram_bytes` reads `/proc/meminfo`
#: and nothing else), so on macOS the budget assertions would skip and prove
#: nothing. And a binding budget is the regime the finding is about: the 1B
#: store ran at 32 GB against 30.79 GB of segments, where the pin table takes
#: almost the whole budget. 4 MiB binds against the fixture's ~7.9 MB of
#: decoded columns while still leaving several entries to choose between,
#: which is what an LRU needs to be under test at all.
CACHE_BUDGET = 4 * 1024 * 1024

_PROBE = r"""
# Build one configuration, report its memory accounting, its resident growth and its
# wall. Run as a subprocess by tests/test_memory_accounting.py, never
# imported.
import json, os, resource, sys, tempfile, time
from pathlib import Path

ROOT = sys.argv[1]
N_ROWS, ROWS_PER_BATCH, CORRECTIONS = (int(a) for a in sys.argv[2:5])
SAMPLE = sys.argv[5] == "sample"
os.environ["GNOMON_SEGMENT_CACHE_BYTES"] = sys.argv[6]
sys.path.insert(0, ROOT)
from gnomon.storage.native import NativeAdapter

# A deliberately added cost in the sampled path, for the test that proves the
# paired cost gate can fail: every `memory_accounting()` call spins the CPU
# for this many microseconds first. Test-only; zero everywhere else.
COST_US = int(os.environ.get("GNOMON_TEST_SAMPLING_COST_US", "0"))
if COST_US:
    _real = NativeAdapter.memory_accounting

    def _costly(self, *a, **k):
        end = time.process_time() + COST_US / 1e6
        while time.process_time() < end:
            pass
        return _real(self, *a, **k)

    NativeAdapter.memory_accounting = _costly


def rss_kb():
    # VmRSS plus, on a kernel that reports them, the anon / file / shmem
    # split that says which half of the accounting a component belongs
    # against. RssShmem matters whenever the store is on tmpfs: its mapped
    # pages land there and RssFile stays near zero.
    out = {}
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith(("VmRSS:", "VmHWM:", "RssAnon:",
                                    "RssFile:", "RssShmem:")):
                    k, v = line.split(":")
                    out[k.strip().lower()] = int(v.split()[0])
    except OSError:
        pass
    ru = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    out["maxrss_kb"] = ru // 1024 if sys.platform == "darwin" else ru
    return out


def _src(v):
    return "n%d" % (v % 500)


def _dst(v):
    return "n%d" % ((v * 7) % 500)


def bulk(vts):
    return [{"op": "ingest_events", "events": [
        {"src": _src(v), "dst": _dst(v), "rel_type": "R", "disc": "#%d" % v,
         "props": {"v": v, "pad": "x" * 64}, "vt_s": v, "vt_e": v + 1}
        for v in vts]}]


def correction(v):
    return [{"op": "correct",
             "ref": {"kind": "edge", "src": _src(v), "dst": _dst(v),
                     "rel_type": "R", "disc": "#%d" % v},
             "props": {"v": v, "pad": "y" * 64}, "vt_s": v, "vt_e": v + 1}]


def main():
    base = rss_kb()
    samples, sample_s = 0, 0.0
    with tempfile.TemporaryDirectory() as td:
        a = NativeAdapter(Path(td) / "s")
        a.paranoid = False  # O(versions) per op, and not what is under test
        t0 = time.perf_counter()
        c0 = time.process_time()
        tt = 1000
        for lo in range(0, N_ROWS, ROWS_PER_BATCH):
            tt += 1
            a.begin()
            a.apply_ops(bulk(range(lo, min(lo + ROWS_PER_BATCH, N_ROWS))), tt)
            a.commit()
            if SAMPLE:  # the progress-line half of the build's cadence
                s0 = time.perf_counter()
                a.memory_accounting()
                sample_s += time.perf_counter() - s0
                samples += 1
        step = max(1, N_ROWS // CORRECTIONS)
        for k in range(CORRECTIONS):
            tt += 1
            a.begin()
            a.apply_ops(correction(k * step), tt)
            a.commit()
            if SAMPLE:  # the per-point-op half
                s0 = time.perf_counter()
                a.memory_accounting()
                sample_s += time.perf_counter() - s0
                samples += 1
        wall = time.perf_counter() - t0
        cpu = time.process_time() - c0
        # Read once regardless of configuration, *after* the timer: the no-sample run
        # still has to report the components, and this one call is outside
        # what the wall comparison measures.
        got = a.memory_accounting()
        peak = rss_kb()
        a.close()
    print(json.dumps({"current": got["current"], "peak": got["peak"],
                      "base_kb": base, "peak_kb": peak, "wall_s": wall, "cpu_s": cpu,
                      "samples": samples, "sample_s": sample_s,
                      "platform": sys.platform}))


main()
"""


def _run(sample: bool, cost_us: int = 0) -> dict:
    env = dict(os.environ)
    env.pop("GNOMON_TEST_SAMPLING_COST_US", None)
    # One BLAS thread in the probe. The probe imports numpy, and
    # a Linux numpy built on OpenBLAS starts one worker per core at import
    # (31 on the 32-core development VM). Those workers are not the engine,
    # but `time.process_time()` counts them: measured beside two running
    # suites, they added 1.0-1.6 s to a window whose own main-thread CPU was
    # 0.91-0.94 s, varying from run to run by more than the 0.56 s the
    # self-test injects -- so a sampled run could come out *cheaper* than its
    # unsampled pair, and the self-test missed its trip on both trees
    # (+1.6 % and +7.2 %). Pinned to one thread, the window's process CPU
    # equals its main-thread CPU (0.91-0.93 s unsampled, 1.47-1.48 s with
    # the injected 8 ms x 70). Engine threads are untouched: the scan workers
    # are std threads sized by GNOMON_SCAN_THREADS, and still counted. macOS
    # numpy uses Accelerate, which keeps no such pool -- why the laptop
    # calibration above never saw this.
    for var in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "MKL_NUM_THREADS"):
        env[var] = "1"
    if cost_us:
        env["GNOMON_TEST_SAMPLING_COST_US"] = str(cost_us)
    with tempfile.TemporaryDirectory() as td:
        probe = Path(td) / "probe.py"
        probe.write_text(_PROBE)
        proc = subprocess.run(
            [sys.executable, str(probe), ROOT, str(N_ROWS),
             str(ROWS_PER_BATCH), str(CORRECTIONS),
             "sample" if sample else "no", str(CACHE_BUDGET)],
            capture_output=True, text=True, timeout=900, check=False, env=env)
    if proc.returncode != 0:
        pytest.fail(f"probe failed ({proc.returncode}):\n{proc.stderr[-4000:]}")
    return json.loads(proc.stdout.strip().splitlines()[-1])


@pytest.fixture(scope="module")
def sampled() -> dict:
    return _run(sample=True)


def test_the_accessor_reports_every_component_it_promises(sampled):
    """A missing key is a component that stopped being attributed."""
    for half in ("current", "peak"):
        assert set(sampled[half]) == EXPECTED, half
        assert all(isinstance(v, int) and v >= 0
                   for v in sampled[half].values()), half


def test_the_fixture_actually_populates_the_cache_and_the_postings(sampled):
    """The reality control: every threshold below is vacuous if the
    run never held anything.

    The point ops are what do it. A pure ingest reads nothing back, so the
    segment cache and the postings both stay empty — which is exactly why the
    1B build's RSS sat flat at 3 GiB for 15.7 h and then ramped inside its
    last 262 ops."""
    cur = sampled["current"]
    assert cur["segment_cache_entries"] > 0, "no segment was ever cached"
    assert cur["segment_cache_rows"] > 0, "cached segments hold no rows"
    assert cur["segment_cache_decoded"] > 0, "nothing was ever decoded"
    assert cur["postings"] > 0, "the postings never folded a segment"
    assert cur["pinned_segments"] > 0, "pinning is off; the mapped identity "\
                                       "below would not be under test"


def test_the_budget_charges_decoded_heap_and_never_the_mapping(sampled):
    """`GNOMON_SEGMENT_CACHE_BYTES` bounds anonymous bytes.

    A segment's file mapping is the store itself -- file-backed pages the
    kernel manages, reclaimable under pressure on a disk-backed store and
    only pinned-*looking* on tmpfs. It is not charged to the budget and is
    not credited back on eviction. Charging it was what made a 32 GB budget
    produce 52.6 GB of segment-attributable residency at 1B. An identity, not
    a tolerance."""
    c = sampled["current"]
    assert c["segment_cache_mapped"] > 0, "nothing mapped; the exclusion is vacuous"
    assert c["segment_cache_accounted"] == (
        c["segment_cache_decoded"] + c["segment_cache_header"])


def test_the_decoded_budget_binds_what_it_says_it_binds(sampled):
    """A budget of B bounds B bytes of decoded heap, never B minus something
    the operator was not told about. The cache never evicts its last entry,
    so one segment's own heap is the whole allowance."""
    c = sampled["current"]
    assert c["segment_cache_budget"] == CACHE_BUDGET
    largest = c["segment_cache_accounted"] // max(1, c["segment_cache_entries"])
    assert c["segment_cache_accounted"] <= CACHE_BUDGET + largest
    assert c["segment_cache_evictions"] > 0, "a binding budget must have evicted"


def test_the_three_classes_partition_what_the_accounting_names(sampled):
    """`anonymous_total` is every heap component summed;
    `file_mapped_total` is the store's own mapped bytes. Reported separately
    because they are bounded by different things and land in different
    `/proc/self/status` fields."""
    c = sampled["current"]
    assert c["anonymous_total"] == sum(c[k] for k in ANON)
    # the store's pinned segments plus the identity filters, which are
    # file mappings too -- still exactly the sum of its named parts
    assert c["file_mapped_total"] == c["pinned_mapped"] + c["vid_filter_mapped"]


def test_every_cached_segments_mapping_is_also_a_pinned_one(sampled):
    """Pinning maps every segment of the open generation, so evicting one from
    the byte-budgeted cache credits back bytes the process does not release.
    `pinned_mapped >= segment_cache_mapped` is what says so."""
    cur = sampled["current"]
    assert cur["pinned_mapped"] >= cur["segment_cache_mapped"]


def test_the_peak_dominates_the_current_reading(sampled):
    """`peak` is a running maximum over the samples taken, so it can never be
    below the last `current` for the same component."""
    cur, peak = sampled["current"], sampled["peak"]
    below = {k: (cur[k], peak[k]) for k in cur if peak[k] < cur[k]}
    assert not below, below


def test_the_components_sum_to_a_stated_share_of_the_resident_growth(sampled):
    """The secondary, looser signal — see the module docstring for why the
    fraction is wide and one-sided."""
    base = sampled["base_kb"].get("vmrss") or sampled["base_kb"]["maxrss_kb"]
    top = sampled["peak_kb"].get("vmrss") or sampled["peak_kb"]["maxrss_kb"]
    growth = (top - base) * 1024
    if growth <= 0:
        pytest.skip("no measurable resident growth on this host")
    cur = sampled["current"]
    accounted = sum(cur[k] for k in ANON) + cur["file_mapped_total"]
    share = accounted / growth
    assert MIN_EXPLAINED <= share <= MAX_EXPLAINED, (
        f"accounted {accounted:,} B against {growth:,} B of growth "
        f"= {share:.3f}, outside [{MIN_EXPLAINED}, {MAX_EXPLAINED}]; "
        f"components={json.dumps(cur, sort_keys=True)}")


def test_sampling_it_on_the_builds_cadence_costs_under_half_a_percent(sampled):
    """The instrument's price, timed inside the configuration that pays it.

    Measured in-process rather than as a wall difference between two
    subprocess configurations: at this fixture's size the whole build is ~2.3 s and the
    between-run spread of two interpreters is tens of milliseconds, which is
    the same order as the quantity under test. A difference of two noisy
    numbers is the wrong instrument for a gate of half a percent; the time
    actually spent inside the accessor is the right one, and it is not
    subject to any noise the comparison would import.

    The cadence is the build's own: once per commit and once per point op."""
    assert sampled["samples"] > 0, "the sampling configuration took no samples"
    share = sampled["sample_s"] / sampled["wall_s"]
    assert share < MAX_WALL_FRACTION, (
        f"{sampled['samples']} samples cost {sampled['sample_s']:.4f}s of "
        f"{sampled['wall_s']:.3f}s = {share:.4%}, over {MAX_WALL_FRACTION:.1%}")


def _paired_overhead(pairs: int, cost_us: int = 0) -> tuple[float, str]:
    """Median over `pairs` interleaved (sampling, non-sampling) runs of the
    ratio of their CPU times, minus one; and a description of the runs."""
    ratios, rows = [], []
    for _ in range(pairs):
        a = _run(sample=True, cost_us=cost_us)
        b = _run(sample=False, cost_us=cost_us)
        assert b["samples"] == 0 and a["samples"] > 0
        ratios.append(a["cpu_s"] / b["cpu_s"])
        rows.append(f"{a['cpu_s']:.3f}/{b['cpu_s']:.3f}")
    return statistics.median(ratios) - 1.0, " ".join(rows)


def test_not_sampling_it_costs_nothing_at_all():
    """The other half of the claim: a build that samples the accessor on its
    cadence costs, from outside the process, what a build that never calls it
    costs. Nothing on any read or write path reads `mem_peaks`, so this is an
    assertion about the absence of a hook, checked the way a black-box test
    can: the two configurations' CPU times, compared pair by pair over interleaved runs
    (see MAX_ARM_CPU_OVERHEAD for why CPU, why pairs, and the band)."""
    overhead, runs = _paired_overhead(ARM_PAIRS)
    assert overhead < MAX_ARM_CPU_OVERHEAD, (
        f"sampling configuration costs {overhead:+.2%} CPU over the non-sampling configuration "
        f"(median of {ARM_PAIRS} interleaved pairs, sampled/unsampled s: {runs}), "
        f"beyond the {MAX_ARM_CPU_OVERHEAD:.0%} band")


def test_the_paired_gate_fails_when_sampling_costs_something():
    """The gate above can fail: with INJECTED_COST_US of CPU added to every
    sampled `memory_accounting()` call (a test-only switch in the probe, no
    engine change), the same statistic must exceed the band. Three pairs are
    enough here -- the injected effect is about four times the band, and
    this test asserts that the gate trips, not where."""
    overhead, runs = _paired_overhead(3, cost_us=INJECTED_COST_US)
    assert overhead > MAX_ARM_CPU_OVERHEAD, (
        f"an added {INJECTED_COST_US} us per sample moved the statistic only "
        f"{overhead:+.2%} (sampled/unsampled s: {runs}); the gate could not "
        f"have caught it")
